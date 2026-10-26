//! A segment of the record index ([RFC 0068]) — the fixed-size table a
//! [`RecordIndex`](crate::record_index::RecordIndex) directory entry names.
//!
//! [`REGULAR`] buckets plus [`STASH`] spill buckets, [`SLOTS`] slots each. A
//! key lives in its home bucket, the one after it, or a stash bucket. A slot
//! holds a [`Loc`] and a one-byte fingerprint; a bucket's fingerprints sit
//! together and are compared in one pass, written as plain code the compiler
//! vectorises — no `unsafe`. Split out of the index for the file budget: the
//! seam is real, this is the table and the index is the directory over it.
//!
//! [RFC 0068]: ../../../rfcs/0068-record-cache-as-a-log-WIP.md

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
pub struct Bucket {
    fps: [u8; LANES],
    pub used: u16,
    pub locs: [Loc; SLOTS],
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

    pub const fn clear(&mut self, at: usize) {
        self.used &= !(1 << at);
    }

    const fn len(&self) -> u32 {
        self.used.count_ones()
    }
}

pub struct Segment {
    /// Local depth: how many top hash bits every key in it shares.
    pub depth: u32,
    pub buckets: [Bucket; BUCKETS],
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
    pub fn boxed(depth: u32) -> Box<Self> {
        Box::new(Self {
            depth,
            buckets: [Bucket::EMPTY; BUCKETS],
        })
    }

    /// The `(bucket, slot)` holding the key `is` recognises, if any.
    pub fn find(
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
    pub fn place(&mut self, hash: u64, loc: Loc) -> bool {
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

#[cfg(test)]
mod tests {
    use super::{Bucket, Loc, SLOTS};

    fn loc(i: usize) -> Loc {
        Loc {
            slot: 0,
            offset: 0,
            chunk: i as u32,
        }
    }

    #[test]
    fn a_probe_offers_only_occupied_slots_with_the_fingerprint() {
        let mut bucket = Bucket::EMPTY;
        bucket.put(0, 9, loc(0));
        bucket.put(3, 9, loc(3));
        bucket.put(5, 4, loc(5));
        assert_eq!(bucket.candidates(9), 0b1001);
        bucket.clear(3);
        assert_eq!(bucket.candidates(9), 0b0001, "a cleared slot is no hit");
        assert_eq!(bucket.candidates(0), 0, "free lanes hold stale zeros");
        for at in 0..SLOTS {
            bucket.put(at, 1, loc(at));
        }
        assert_eq!(bucket.free(), None);
    }
}
