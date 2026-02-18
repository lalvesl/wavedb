//! The chain segment ([RFC 0050]) — a doubly-linked run of sorted entries,
//! stored as one ordinary value under its own [`LocalId`].
//!
//! A collection's living records are additionally stored inline in a chain of
//! these, so a scan costs one read per segment instead of one page read (and one
//! zstd decompression) per record. This module is the **value**: its byte form,
//! its lane identity, and the in-place edits a chain performs on it. Locating a
//! segment, splitting it and keeping the sparse index in step live above, over
//! the [`Store`](crate::Store).
//!
//! ## Value format
//!
//! ```text
//! [ lane hash (8 B LE) ][ WaveWire (prev, next, entries) ]
//! ```
//!
//! The leading 8 bytes are the lane's `STRUCT_HASH` — storage backends route
//! every stored value by the hash in its first 8 bytes, and a segment is an
//! ordinary value to any `Store`. No kind byte: every chain shares one encoding,
//! and which chain a segment belongs to is decided by the `Pivot` root that
//! reaches it.
//!
//! ## Keys and payloads
//!
//! Every chain keys by [`SecKey`] — the modification-ordered chain puts the live
//! version's authoring instant in `field` (byte-for-byte what the `recency` tree
//! did), a declared list puts the encoded field value there. What varies is
//! the payload: record bytes in a record lane, `()` in the removal log, where the
//! anchor inside the key is the whole entry.
//!
//! [RFC 0050]: https://github.com/wavedb/wavedb/blob/main/rfcs/0050-clustered-record-chains.md

use wavedb_wire::Cursor;

use crate::error::{Error, Result};
use crate::local_id::LocalId;
use crate::wire::{WaveWire, from_wire, to_wire};

use super::sec_key::SecKey;

/// Bytes before the payload: the lane's `STRUCT_HASH` tag.
const LANE_PREFIX: usize = 8;

/// Which reserved lane a chain's segments live in.
///
/// One directory per lane per user type, so a page stays homogeneous and the
/// per-type zstd dictionary models one kind of content. Records and removals
/// separate because their contents, lifetimes and temperatures all differ: the
/// record lanes are bounded by the live set and hot, the removal log grows
/// forever and is read almost never.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// **Declared list** chains ([RFC 0051]) — the only segments that carry
    /// records. Payload = the record's stored envelope, verbatim.
    ///
    /// [RFC 0051]: https://github.com/wavedb/wavedb/blob/main/rfcs/0051-ordered-record-lists.md
    Records,
    /// The **recency** chain: one id per living record, keyed by its live
    /// version's instant. Payload = `()`.
    ///
    /// Its own lane rather than sharing the record one, for the reason lanes
    /// exist: an ~18-byte id entry and a segment of whole records are different
    /// content, and a per-type zstd dictionary can only model one of them well.
    /// It is the removal log's twin in every respect but the question it
    /// answers, and it is filed like one.
    Recency,
    /// The removal log. Payload = `()`.
    Dead,
    /// Sparse-index nodes — the descent above a chain.
    ///
    /// Its own lane because index nodes are *navigational* (small, reread
    /// constantly) while segments are *streaming* (large, touched once): the
    /// distinction RFC 0053 draws, which only separate lanes let the cache and
    /// the bucket target act on.
    Index,
    /// `#[wavedb::pivot(...)]` secondary chains ([RFC 0075]): one
    /// `[field][anchor]` key per record and no payload.
    ///
    /// Its own lane rather than the recency chain's: a field-keyed pointer
    /// is different content from an instant-keyed one, and a lane is what
    /// lets one dictionary model one kind of thing.
    ///
    /// [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains-WIP.md
    Secondary,
    /// `#[wavedb::fuzzy]` posting chains ([RFC 0075]): one `[gram][len][anchor]`
    /// key per gram of a record's value, and no payload.
    ///
    /// [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains-WIP.md
    Fuzzy,
}

impl Lane {
    /// This lane's reserved `STRUCT_HASH` for the user type `struct_hash`.
    ///
    /// Derived, not declared, so no schema carries it: SeaHash (the pinned
    /// identity hash) over a per-lane tag and the type's own hash. A real struct
    /// hashing to the same value is a 2⁻⁶⁴ event and would merely share the
    /// directory, harmlessly.
    #[must_use]
    pub fn hash(self, struct_hash: u64) -> u64 {
        let tag: &[u8] = match self {
            Self::Records => b"WDB.SEG",
            Self::Recency => b"WDB.REC",
            Self::Dead => b"WDB.DEAD",
            Self::Index => b"WDB.IDX",
            Self::Secondary => b"WDB.SEC",
            Self::Fuzzy => b"WDB.FZY",
        };
        let mut bytes = tag.to_vec();
        bytes.extend_from_slice(&struct_hash.to_le_bytes());
        seahash::hash(&bytes)
    }
}

/// Mint a fresh `LocalId` in `lane_hash`'s lane — a segment or an index node.
///
/// A [`key_nanos`] key (collision-free by its fused counter), `FLAG = 1`, and the
/// lane hash's type salt — which keeps lane ids apart from record anchors,
/// archive slots and tree nodes even in a flat keyspace (IndexedDB).
///
/// [`key_nanos`]: wavedb_platform::time::key_nanos
#[must_use]
pub fn mint_lane_id(lane_hash: u64) -> LocalId {
    LocalId::new(
        wavedb_platform::time::key_nanos(),
        true,
        crate::record::type_salt(lane_hash),
    )
}

/// One segment of a chain: entries sorted by [`SecKey`], plus the ids of the
/// neighbours on either side.
///
/// `prev` runs toward smaller keys (`None` = this is the chain's head), `next`
/// toward larger ones (`None` = the tail).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment<P> {
    prev: Option<LocalId>,
    next: Option<LocalId>,
    entries: Vec<(SecKey, P)>,
}

impl<P> Segment<P> {
    /// An empty segment with the given neighbours.
    #[must_use]
    pub const fn new(prev: Option<LocalId>, next: Option<LocalId>) -> Self {
        Self {
            prev,
            next,
            entries: Vec::new(),
        }
    }

    /// A segment holding `entries` — which must already ascend by key, as they
    /// do when they came out of another segment.
    #[must_use]
    pub const fn with_entries(
        prev: Option<LocalId>,
        next: Option<LocalId>,
        entries: Vec<(SecKey, P)>,
    ) -> Self {
        Self {
            prev,
            next,
            entries,
        }
    }

    /// Take every entry out, leaving the segment empty but still linked — how a
    /// split and a merge move runs of entries without copying them.
    pub fn take_entries(&mut self) -> Vec<(SecKey, P)> {
        core::mem::take(&mut self.entries)
    }

    /// Append `entries`, which must all sort above everything already here (a
    /// merge with the neighbour toward larger keys).
    pub fn extend(&mut self, entries: Vec<(SecKey, P)>) {
        self.entries.extend(entries);
    }

    /// The neighbour toward smaller keys; `None` if this is the head.
    #[must_use]
    pub const fn prev(&self) -> Option<LocalId> {
        self.prev
    }

    /// The neighbour toward larger keys; `None` if this is the tail.
    #[must_use]
    pub const fn next(&self) -> Option<LocalId> {
        self.next
    }

    /// Re-point the neighbour toward smaller keys.
    pub const fn set_prev(&mut self, prev: Option<LocalId>) {
        self.prev = prev;
    }

    /// Re-point the neighbour toward larger keys.
    pub const fn set_next(&mut self, next: Option<LocalId>) {
        self.next = next;
    }

    /// How many entries this segment holds — what the sparse index records as
    /// its count, and what the `N…2N` band bounds.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// `true` when this segment holds nothing. An emptied chain keeps its last
    /// segment as an empty shell so the head and tail ids survive.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The least key present — the separator the sparse index files this segment
    /// under.
    #[must_use]
    pub fn first_key(&self) -> Option<&SecKey> {
        self.entries.first().map(|(k, _)| k)
    }

    /// The greatest key present. The tail segment's is the chain's watermark,
    /// which is where the collection's instant floor comes from.
    #[must_use]
    pub fn last_key(&self) -> Option<&SecKey> {
        self.entries.last().map(|(k, _)| k)
    }

    /// The entries in key order — what a scan yields.
    pub fn entries(&self) -> impl Iterator<Item = &(SecKey, P)> {
        self.entries.iter()
    }

    /// The payload filed under `key`.
    #[must_use]
    pub fn get(&self, key: &SecKey) -> Option<&P> {
        self.position(key).ok().map(|i| &self.entries[i].1)
    }

    /// Insert or replace `key`'s entry, keeping the run sorted. Returns the
    /// payload it displaced, if any.
    pub fn insert(&mut self, key: SecKey, payload: P) -> Option<P> {
        match self.position(&key) {
            Ok(i) => Some(core::mem::replace(&mut self.entries[i].1, payload)),
            Err(i) => {
                self.entries.insert(i, (key, payload));
                None
            }
        }
    }

    /// Remove `key`'s entry, returning its payload if it was present.
    pub fn remove(&mut self, key: &SecKey) -> Option<P> {
        self.position(key).ok().map(|i| self.entries.remove(i).1)
    }

    /// Split at `at`, keeping `..at` here and returning `at..`.
    ///
    /// Only the entries move: re-linking and minting the new segment's id belong
    /// to the chain, which is what lets a split always hand the **new** id to the
    /// interior side and leave a head's or tail's id permanent.
    pub fn split_off(&mut self, at: usize) -> Vec<(SecKey, P)> {
        if at >= self.entries.len() {
            return Vec::new();
        }
        self.entries.split_off(at)
    }

    /// Where `key` sits: `Ok(i)` when present, `Err(i)` where it would go.
    fn position(&self, key: &SecKey) -> core::result::Result<usize, usize> {
        self.entries.binary_search_by(|(k, _)| k.cmp(key))
    }
}

// Hand impl rather than a derive: the derive would need an owned wire-shape
// twin, and copying a segment's entries to encode them would double the bytes a
// write already moves. The composition is exactly the derive's — each field's
// stack slots inline in order, heaps appended in the same order — so this is the
// tuple `(prev, next, entries)` layout without owning the tuple.
impl<P: WaveWire> WaveWire for Segment<P> {
    const STACK_SIZE: usize = <Option<LocalId> as WaveWire>::STACK_SIZE * 2
        + <Vec<(SecKey, P)> as WaveWire>::STACK_SIZE;

    fn heap_size(&self) -> usize {
        self.prev.heap_size() + self.next.heap_size() + self.entries.heap_size()
    }

    fn encode_stack(&self, stack: &mut Vec<u8>) {
        self.prev.encode_stack(stack);
        self.next.encode_stack(stack);
        self.entries.encode_stack(stack);
    }

    fn encode_heap(&self, heap: &mut Vec<u8>) {
        self.prev.encode_heap(heap);
        self.next.encode_heap(heap);
        self.entries.encode_heap(heap);
    }

    fn decode(
        stack: &mut Cursor,
        heap: &mut Cursor,
    ) -> wavedb_wire::Result<Self> {
        Ok(Self {
            prev: Option::decode(stack, heap)?,
            next: Option::decode(stack, heap)?,
            entries: Vec::decode(stack, heap)?,
        })
    }
}

impl<P: WaveWire> Segment<P> {
    /// Serialise: the lane tag, then the `WaveWire` payload.
    #[must_use]
    pub fn to_bytes(&self, lane_hash: u64) -> Vec<u8> {
        let mut out = lane_hash.to_le_bytes().to_vec();
        out.extend_from_slice(&to_wire(self));
        out
    }

    /// Parse a segment value, checking the lane tag first.
    ///
    /// # Errors
    /// [`Error::LaneBadTag`] if the first 8 bytes are not `lane_hash` (or the
    /// value is shorter than the tag) — the pointer resolved to some other kind
    /// of value; [`Error::Wire`] if the payload fails to decode.
    pub fn from_bytes(lane_hash: u64, buf: &[u8]) -> Result<Self> {
        let tag_bytes: [u8; LANE_PREFIX] = buf
            .get(..LANE_PREFIX)
            .and_then(|s| s.try_into().ok())
            .ok_or(Error::LaneBadTag(0))?;
        let tag = u64::from_le_bytes(tag_bytes);
        if tag != lane_hash {
            return Err(Error::LaneBadTag(tag));
        }
        Ok(from_wire::<Self>(&buf[LANE_PREFIX..])?)
    }
}

#[cfg(test)]
mod tests {
    use super::{Lane, Segment, mint_lane_id};
    use crate::error::Error;
    use crate::index::sec_key::SecKey;
    use crate::local_id::LocalId;

    const TYPE_HASH: u64 = 0xABCD_0123_4567_89AB;

    fn key(instant: u64, rec: u64) -> SecKey {
        SecKey {
            field: instant.to_be_bytes().to_vec(),
            rec: LocalId::new(rec, false, 7),
        }
    }

    fn filled(instants: &[u64]) -> Segment<Vec<u8>> {
        let mut seg = Segment::new(None, None);
        for &i in instants {
            seg.insert(key(i, i), vec![i as u8]);
        }
        seg
    }

    #[test]
    fn a_record_segment_roundtrips_through_its_lane() {
        let hash = Lane::Records.hash(TYPE_HASH);
        let mut seg = filled(&[10, 20, 30]);
        seg.set_prev(Some(LocalId::new(1, true, 2)));
        seg.set_next(Some(LocalId::new(3, true, 4)));

        let bytes = seg.to_bytes(hash);
        assert_eq!(Segment::from_bytes(hash, &bytes).unwrap(), seg);
    }

    #[test]
    fn a_payload_free_segment_roundtrips_for_the_removal_log() {
        let hash = Lane::Dead.hash(TYPE_HASH);
        let mut seg: Segment<()> = Segment::new(None, None);
        seg.insert(key(5, 5), ());
        seg.insert(key(9, 9), ());

        let bytes = seg.to_bytes(hash);
        assert_eq!(Segment::<()>::from_bytes(hash, &bytes).unwrap(), seg);
    }

    #[test]
    fn a_foreign_lane_tag_is_refused() {
        let bytes = filled(&[1]).to_bytes(Lane::Records.hash(TYPE_HASH));
        let dead = Lane::Dead.hash(TYPE_HASH);

        // Right shape, wrong lane: the value decodes fine but is not ours.
        assert!(matches!(
            Segment::<Vec<u8>>::from_bytes(dead, &bytes),
            Err(Error::LaneBadTag(t)) if t == Lane::Records.hash(TYPE_HASH)
        ));
    }

    #[test]
    fn a_value_shorter_than_the_tag_is_refused() {
        assert!(matches!(
            Segment::<Vec<u8>>::from_bytes(7, &[0, 1, 2]),
            Err(Error::LaneBadTag(0))
        ));
    }

    #[test]
    fn a_truncated_payload_is_a_wire_fault() {
        let hash = Lane::Records.hash(TYPE_HASH);
        let bytes = filled(&[1, 2, 3]).to_bytes(hash);
        let cut = &bytes[..bytes.len() - 3];

        assert!(matches!(
            Segment::<Vec<u8>>::from_bytes(hash, cut),
            Err(Error::Wire(_))
        ));
    }

    #[test]
    fn lanes_and_types_never_share_a_hash() {
        let other = 0x0123_4567_89AB_CDEF;
        let hashes = [
            Lane::Records.hash(TYPE_HASH),
            Lane::Dead.hash(TYPE_HASH),
            Lane::Records.hash(other),
            Lane::Dead.hash(other),
        ];
        for (i, a) in hashes.iter().enumerate() {
            assert_ne!(*a, TYPE_HASH, "a lane must not collide with its type");
            for b in &hashes[i + 1..] {
                assert_ne!(a, b, "lanes and types must occupy distinct hashes");
            }
        }
    }

    #[test]
    fn a_lane_hash_is_stable_across_calls() {
        // The lane is an address derivation, so it must be a pure function of
        // the type hash — a per-run value would strand every stored segment.
        assert_eq!(
            Lane::Records.hash(TYPE_HASH),
            Lane::Records.hash(TYPE_HASH)
        );
    }

    #[test]
    fn segment_ids_are_minted_apart_and_never_repeat() {
        let hash = Lane::Records.hash(TYPE_HASH);
        let a = mint_lane_id(hash);
        let b = mint_lane_id(hash);
        assert_ne!(a, b, "the fused counter must order back-to-back mints");
        assert_ne!(
            a.salt(),
            mint_lane_id(Lane::Dead.hash(TYPE_HASH)).salt(),
            "lanes must land in different salt lanes"
        );
    }

    #[test]
    fn inserts_land_in_key_order_whatever_the_arrival_order() {
        let seg = filled(&[30, 10, 20]);
        let order: Vec<_> =
            seg.entries().map(|(k, _)| k.field.clone()).collect();
        assert_eq!(
            order,
            vec![
                10u64.to_be_bytes().to_vec(),
                20u64.to_be_bytes().to_vec(),
                30u64.to_be_bytes().to_vec()
            ]
        );
        assert_eq!(seg.first_key(), Some(&key(10, 10)));
        assert_eq!(seg.last_key(), Some(&key(30, 30)));
    }

    #[test]
    fn inserting_a_present_key_replaces_and_hands_back_the_old_payload() {
        let mut seg = filled(&[10]);
        assert_eq!(seg.insert(key(10, 10), vec![99]), Some(vec![10]));
        assert_eq!(seg.len(), 1, "a replace must not grow the segment");
        assert_eq!(seg.get(&key(10, 10)), Some(&vec![99]));
    }

    #[test]
    fn removing_reports_whether_the_key_was_there() {
        let mut seg = filled(&[10, 20]);
        assert_eq!(seg.remove(&key(10, 10)), Some(vec![10]));
        assert_eq!(seg.remove(&key(10, 10)), None);
        assert_eq!(seg.len(), 1);
    }

    #[test]
    fn an_empty_segment_still_answers_every_query() {
        let seg: Segment<Vec<u8>> = Segment::new(None, None);
        assert!(seg.is_empty());
        assert_eq!(seg.first_key(), None);
        assert_eq!(seg.last_key(), None);
        assert_eq!(seg.get(&key(1, 1)), None);
    }

    #[test]
    fn a_split_keeps_the_lower_half_and_hands_back_the_upper() {
        let mut seg = filled(&[10, 20, 30, 40]);
        let upper = seg.split_off(2);

        assert_eq!(seg.len(), 2);
        assert_eq!(seg.last_key(), Some(&key(20, 20)));
        let moved: Vec<_> = upper.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(moved, vec![key(30, 30), key(40, 40)]);
    }

    #[test]
    fn a_split_past_the_end_moves_nothing() {
        let mut seg = filled(&[10, 20]);
        let observed = seg.split_off(2);
        assert!(observed.is_empty(), "{observed:?}");
        assert_eq!(seg.len(), 2, "the segment must be left intact");
    }

    #[test]
    fn the_links_survive_a_roundtrip_independently() {
        let hash = Lane::Records.hash(TYPE_HASH);
        // A head has no `prev`; a tail has no `next`. Both ends must decode as
        // `None` rather than as some sentinel id.
        let mut head: Segment<Vec<u8>> =
            Segment::new(None, Some(LocalId::new(9, true, 1)));
        head.insert(key(1, 1), vec![]);
        let bytes = head.to_bytes(hash);
        let back = Segment::<Vec<u8>>::from_bytes(hash, &bytes).unwrap();

        assert_eq!(back.prev(), None);
        assert_eq!(back.next(), Some(LocalId::new(9, true, 1)));
    }
}
