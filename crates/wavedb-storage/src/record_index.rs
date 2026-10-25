//! The record index ([RFC 0068]) — where a cached record's newest entry
//! sits in its type's log.
//!
//! Dragonfly's Dashtable, in shape: a **directory** (a `Vec<u32>` of segment
//! numbers, indexed by the hash's top bits) over **segments** of fixed size.
//! A segment is a flat table of [`REGULAR`] buckets plus [`STASH`] spill
//! buckets, [`SLOTS`] slots each. A key lives in its home bucket, the one
//! after it, or a stash bucket; when all of them are full, **that segment
//! alone** splits in two, and the directory at most doubles. Growth is one
//! segment at a time, so there is no rehash of the whole table and no
//! transient copy of it.
//!
//! A slot holds only a [`Loc`] — which type's log, which chunk, which offset —
//! and a one-byte fingerprint. The `Id` is not repeated here: it is at the
//! head of the entry the slot names, so a probe that matches a fingerprint
//! confirms the candidate by asking the caller (`is`), which reads the log.
//! The type is the `Loc`'s `slot`, so `(Id, type)` is the key at no extra
//! cost. Every operation that needs a stored key's hash — a split — asks the
//! caller too (`rehash`). The index knows nothing about logs.
//!
//! A bucket's fingerprints sit together and are compared in one pass, written
//! as plain code the compiler vectorises; no `unsafe`.
//!
//! The hash is seeded per open store: a `#[wavedb::key]` id is a hash of
//! client data, and an unseeded table would let a client choose collisions
//! and force splits. Keys that a split cannot separate anyway — the directory
//! may not outgrow its segments by more than [`DIR_SLACK`] doublings — go to
//! a small overflow list, so no input can make an insert loop or the
//! directory explode.
//!
//! [RFC 0068]: ../../../rfcs/0068-record-cache-as-a-log-WIP.md

use std::mem::size_of;

/// Regular buckets per segment — a key's home is one of these.
const REGULAR: usize = 56;
/// Spill buckets per segment, shared by all of its regular buckets.
const STASH: usize = 4;
const BUCKETS: usize = REGULAR + STASH;
/// Slots per bucket.
const SLOTS: usize = 14;
/// A bucket's fingerprint row, padded to one 16-byte vector.
const LANES: usize = 16;
/// The `used` bits of a full bucket.
const FULL: u16 = (1 << SLOTS) - 1;
/// How many times larger than its segment count the directory may grow: a
/// split that would double it past that spills to the overflow list instead.
const DIR_SLACK: u32 = 4;
/// A hard ceiling on the directory's depth, whatever the slack allows.
const MAX_DEPTH: u32 = 32;

/// Where an entry sits: the type's slot in the store, the chunk within that
/// type's log, and the entry's offset in the chunk.
///
/// `offset` fits 16 bits because an entry starts inside a chunk of at most
/// 64 KiB; a record larger than that gets a chunk of its own, at offset 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Loc {
    pub slot: u16,
    pub offset: u16,
    pub chunk: u32,
}

#[derive(Clone, Copy)]
struct Bucket {
    fps: [u8; LANES],
    used: u16,
    locs: [Loc; SLOTS],
}

impl Bucket {
    const EMPTY: Self = Self {
        fps: [0; LANES],
        used: 0,
        locs: [Loc {
            slot: 0,
            offset: 0,
            chunk: 0,
        }; SLOTS],
    };

    /// The occupied slots whose fingerprint is `fp`, as a bit mask — one
    /// compare across the row, vectorised by the compiler.
    fn candidates(&self, fp: u8) -> u16 {
        let mut hits = 0u16;
        for (lane, &f) in self.fps.iter().enumerate() {
            hits |= u16::from(f == fp) << lane;
        }
        hits & self.used
    }

    const fn free(&self) -> Option<usize> {
        let free = !self.used & FULL;
        if free == 0 {
            None
        } else {
            Some(free.trailing_zeros() as usize)
        }
    }

    const fn put(&mut self, at: usize, fp: u8, loc: Loc) {
        self.fps[at] = fp;
        self.locs[at] = loc;
        self.used |= 1 << at;
    }

    const fn clear(&mut self, at: usize) {
        self.used &= !(1 << at);
    }

    const fn len(&self) -> u32 {
        self.used.count_ones()
    }
}

struct Segment {
    /// Local depth: how many top hash bits every key in it shares.
    depth: u32,
    buckets: [Bucket; BUCKETS],
}

const fn fingerprint(hash: u64) -> u8 {
    hash as u8
}

/// The buckets a key may sit in, in probe order: home, neighbour, stash.
const fn probe_order(hash: u64) -> [usize; 2 + STASH] {
    let home = ((hash >> 8) as u32 as usize) % REGULAR;
    [
        home,
        (home + 1) % REGULAR,
        REGULAR,
        REGULAR + 1,
        REGULAR + 2,
        REGULAR + 3,
    ]
}

impl Segment {
    fn boxed(depth: u32) -> Box<Self> {
        Box::new(Self {
            depth,
            buckets: [Bucket::EMPTY; BUCKETS],
        })
    }

    /// The `(bucket, slot)` holding the key `is` recognises, if any.
    fn find(
        &self,
        hash: u64,
        is: &mut impl FnMut(Loc) -> bool,
    ) -> Option<(usize, usize)> {
        let fp = fingerprint(hash);
        for b in probe_order(hash) {
            let bucket = &self.buckets[b];
            let mut hits = bucket.candidates(fp);
            while hits != 0 {
                let at = hits.trailing_zeros() as usize;
                hits &= hits - 1;
                if is(bucket.locs[at]) {
                    return Some((b, at));
                }
            }
        }
        None
    }

    /// Place a new key: the emptier of home and neighbour, else the first
    /// stash bucket with room. `false` when the segment has no room for it.
    fn place(&mut self, hash: u64, loc: Loc) -> bool {
        let fp = fingerprint(hash);
        let [home, next, stash @ ..] = probe_order(hash);
        let (first, second) =
            if self.buckets[next].len() < self.buckets[home].len() {
                (next, home)
            } else {
                (home, next)
            };
        let regular: [usize; 2] = (first, second).into();
        for b in regular.into_iter().chain(stash) {
            if let Some(at) = self.buckets[b].free() {
                self.buckets[b].put(at, fp, loc);
                return true;
            }
        }
        false
    }
}

/// The index (see the module docs).
pub struct RecordIndex {
    seed: [u64; 4],
    /// Global depth: the directory has `1 << depth` entries.
    depth: u32,
    dir: Vec<u32>,
    segments: Vec<Box<Segment>>,
    /// Keys no split could make room for (see the module docs). Empty in
    /// any ordinary run; scanned only when it is not.
    overflow: Vec<(u64, Loc)>,
    len: usize,
}

impl std::fmt::Debug for RecordIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordIndex")
            .field("len", &self.len)
            .field("depth", &self.depth)
            .field("segments", &self.segments.len())
            .field("overflow", &self.overflow.len())
            .finish_non_exhaustive()
    }
}

impl RecordIndex {
    /// An empty index hashing with `seed`: one segment, a one-entry
    /// directory.
    pub fn new(seed: [u64; 4]) -> Self {
        Self {
            seed,
            depth: 0,
            dir: vec![0],
            segments: vec![Segment::boxed(0)],
            overflow: Vec::new(),
            len: 0,
        }
    }

    /// The hash `id` is indexed under.
    pub const fn hash(&self, id: u128) -> u64 {
        crate::directory::hash_of(id, self.seed)
    }

    /// The seed [`hash`](Self::hash) uses — for a caller that must hash
    /// while it holds the index mutably.
    pub const fn seed(&self) -> [u64; 4] {
        self.seed
    }

    /// Keys indexed.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Bytes the index holds: its segments, directory and overflow list.
    pub const fn footprint(&self) -> usize {
        self.segments.len() * size_of::<Segment>()
            + self.segments.capacity() * size_of::<Box<Segment>>()
            + self.dir.capacity() * size_of::<u32>()
            + self.overflow.capacity() * size_of::<(u64, Loc)>()
    }

    fn segment_of(&self, hash: u64) -> usize {
        let at = if self.depth == 0 {
            0
        } else {
            (hash >> (64 - self.depth)) as usize
        };
        self.dir[at] as usize
    }

    /// The location of the key `is` recognises among those hashed `hash`.
    pub fn get(
        &self,
        hash: u64,
        mut is: impl FnMut(Loc) -> bool,
    ) -> Option<Loc> {
        let seg = &self.segments[self.segment_of(hash)];
        if let Some((b, at)) = seg.find(hash, &mut is) {
            return Some(seg.buckets[b].locs[at]);
        }
        self.overflow
            .iter()
            .find(|(h, loc)| *h == hash && is(*loc))
            .map(|(_, loc)| *loc)
    }

    /// Point the key `is` recognises at `loc`, inserting it when absent;
    /// the location it replaced, if any. `rehash` gives the hash of the key
    /// at a stored location, for the splits an insert may cause.
    pub fn upsert(
        &mut self,
        hash: u64,
        loc: Loc,
        mut is: impl FnMut(Loc) -> bool,
        mut rehash: impl FnMut(Loc) -> u64,
    ) -> Option<Loc> {
        let s = self.segment_of(hash);
        if let Some((b, at)) = self.segments[s].find(hash, &mut is) {
            let slot = &mut self.segments[s].buckets[b].locs[at];
            return Some(std::mem::replace(slot, loc));
        }
        if let Some(entry) = self
            .overflow
            .iter_mut()
            .find(|(h, old)| *h == hash && is(*old))
        {
            return Some(std::mem::replace(&mut entry.1, loc));
        }
        self.len += 1;
        loop {
            let s = self.segment_of(hash);
            if self.segments[s].place(hash, loc) {
                return None;
            }
            if !self.split(s, hash, &mut rehash) {
                self.overflow.push((hash, loc));
                return None;
            }
        }
    }

    /// Remove the key `is` recognises; the location it named.
    pub fn remove(
        &mut self,
        hash: u64,
        mut is: impl FnMut(Loc) -> bool,
    ) -> Option<Loc> {
        let s = self.segment_of(hash);
        if let Some((b, at)) = self.segments[s].find(hash, &mut is) {
            let bucket = &mut self.segments[s].buckets[b];
            bucket.clear(at);
            self.len -= 1;
            return Some(bucket.locs[at]);
        }
        let at = self
            .overflow
            .iter()
            .position(|(h, loc)| *h == hash && is(*loc))?;
        self.len -= 1;
        Some(self.overflow.swap_remove(at).1)
    }

    /// Split segment `s` — the one `hash` routes to — moving the keys whose
    /// next hash bit is set into a new segment. `false` when the directory
    /// may not double for it (see [`DIR_SLACK`]).
    fn split(
        &mut self,
        s: usize,
        hash: u64,
        rehash: &mut impl FnMut(Loc) -> u64,
    ) -> bool {
        let d = self.segments[s].depth;
        if d == self.depth {
            let cap = self.segments.len().ilog2() + 1 + DIR_SLACK;
            if self.depth >= MAX_DEPTH.min(cap) {
                return false;
            }
            self.dir = self.dir.iter().flat_map(|&n| [n, n]).collect();
            self.depth += 1;
        }
        let mut fresh = Segment::boxed(d + 1);
        let old = &mut self.segments[s];
        old.depth = d + 1;
        let bit = 63 - d;
        for bucket in &mut old.buckets {
            let mut used = bucket.used;
            while used != 0 {
                let at = used.trailing_zeros() as usize;
                used &= used - 1;
                let loc = bucket.locs[at];
                let h = rehash(loc);
                if (h >> bit) & 1 == 1 {
                    bucket.clear(at);
                    if !fresh.place(h, loc) {
                        self.overflow.push((h, loc));
                    }
                }
            }
        }
        let fresh_no = self.segments.len() as u32;
        self.segments.push(fresh);
        // The directory entries naming `s` are one aligned span; its upper
        // half — the keys whose bit `bit` is set — now names the new segment.
        let span = 1usize << (self.depth - d);
        let start = ((hash >> (64 - self.depth)) as usize) & !(span - 1);
        for entry in &mut self.dir[start + span / 2..start + span] {
            *entry = fresh_no;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{Bucket, Loc, RecordIndex, SLOTS};

    /// A table of keys by location — what the log is to the real index.
    struct Keys(Vec<u64>);

    impl Keys {
        fn loc(i: usize) -> Loc {
            Loc {
                slot: 0,
                offset: 0,
                chunk: i as u32,
            }
        }

        fn insert(&mut self, index: &mut RecordIndex, hash: u64) -> Loc {
            let loc = Self::loc(self.0.len());
            self.0.push(hash);
            let replaced = index.upsert(
                hash,
                loc,
                |l| l == loc,
                |l| self.0[l.chunk as usize],
            );
            assert_eq!(replaced, None);
            loc
        }
    }

    fn seeded() -> RecordIndex {
        RecordIndex::new([1, 2, 3, 4])
    }

    fn assert_all(index: &RecordIndex, keys: &Keys) {
        for (i, &h) in keys.0.iter().enumerate() {
            let loc = Keys::loc(i);
            assert_eq!(index.get(h, |l| l == loc), Some(loc), "key {i}");
        }
    }

    #[test]
    fn many_keys_split_segments_and_stay_reachable() {
        let mut index = seeded();
        let mut keys = Keys(Vec::new());
        for id in 0..20_000u128 {
            let hash = index.hash(id);
            keys.insert(&mut index, hash);
        }
        assert_eq!(index.len(), 20_000);
        assert!(index.segments.len() > 20, "{index:?}");
        assert!(index.overflow.is_empty(), "a seeded hash never overflows");
        assert_all(&index, &keys);
        let per_key = index.footprint() / index.len();
        assert!(per_key < 16, "{per_key} B per key");
    }

    #[test]
    fn an_upsert_repoints_and_a_remove_forgets() {
        let mut index = seeded();
        let h = index.hash(7);
        let (a, b) = (Keys::loc(1), Keys::loc(2));
        let same_key = |l: Loc| l == a || l == b;
        assert_eq!(index.upsert(h, a, same_key, |_| h), None);
        assert_eq!(index.upsert(h, b, same_key, |_| h), Some(a));
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(h, same_key), Some(b));
        assert_eq!(index.remove(h, |l| l == a), None, "names b, not a");
        assert_eq!(index.remove(h, same_key), Some(b));
        assert_eq!(index.get(h, same_key), None);
        assert_eq!(index.len(), 0);
    }

    /// Keys that share a home bucket fill it, its neighbour and the stash;
    /// the segment then splits on the top bits, which still differ.
    #[test]
    fn keys_skewed_onto_one_bucket_split_the_segment() {
        let mut index = seeded();
        let mut keys = Keys(Vec::new());
        for top in 0..2_000u64 {
            // Same home bucket (bits 8..40) and fingerprint; the top bits
            // distinct and spread, as a seeded hash spreads them.
            let spread = top.reverse_bits() & !((1 << 40) - 1);
            keys.insert(&mut index, spread | 0x42);
        }
        assert!(index.segments.len() > 10, "{index:?}");
        assert!(index.overflow.is_empty(), "{index:?}");
        assert_all(&index, &keys);
    }

    /// Identical hashes cannot be separated by any split: they spill to the
    /// overflow list after a bounded number of tries, instead of looping or
    /// doubling the directory without end.
    #[test]
    fn identical_hashes_overflow_instead_of_splitting_forever() {
        let mut index = seeded();
        let mut keys = Keys(Vec::new());
        for _ in 0..500 {
            keys.insert(&mut index, 0xDEAD_BEEF);
        }
        assert_ne!(index.overflow.len(), 0);
        assert!(index.dir.len() <= 1 << 12, "{index:?}");
        assert_all(&index, &keys);
        let loc = Keys::loc(499);
        assert_eq!(index.remove(0xDEAD_BEEF, |l| l == loc), Some(loc));
        assert_eq!(index.len(), 499);
    }

    #[test]
    fn a_probe_offers_only_occupied_slots_with_the_fingerprint() {
        let mut bucket = Bucket::EMPTY;
        bucket.put(0, 9, Keys::loc(0));
        bucket.put(3, 9, Keys::loc(3));
        bucket.put(5, 4, Keys::loc(5));
        assert_eq!(bucket.candidates(9), 0b1001);
        bucket.clear(3);
        assert_eq!(bucket.candidates(9), 0b0001, "a cleared slot is no hit");
        assert_eq!(bucket.candidates(0), 0, "free lanes hold stale zeros");
        for at in 0..SLOTS {
            bucket.put(at, 1, Keys::loc(at));
        }
        assert_eq!(bucket.free(), None);
    }
}
