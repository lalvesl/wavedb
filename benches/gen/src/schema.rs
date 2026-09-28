//! The one benchmark record, and the deterministic generator that fills it.
//!
//! The same logical rows reach every system (RFC 0060 §3), which is what makes
//! the storage-amplification ratio one number on one scale: the denominator is
//! the summed wire size of these records, identical everywhere.

use wavedb_macros::wavedb;

/// A record of realistic shape: scalars, two short strings, one longer text
/// field, and one secondary-indexed field standing in for the SQL index.
#[wavedb(NonUnique)]
#[wavedb::pivot(tag)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Thing {
    pub kind: u32,
    pub score: u64,
    pub name: String,
    pub tag: String,
    pub body: String,
}

/// `SplitMix64` — small, fast, and stable across toolchains, so a seed really
/// does reproduce a dataset (which a derivation-cached seed depends on).
pub struct Rng(u64);

impl Rng {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub const fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`.
    pub const fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// The `n`th record of the dataset — a pure function of `n` and `seed`, so any
/// system can be refilled identically without carrying the rows around.
#[must_use]
pub fn thing(n: u64, seed: u64) -> Thing {
    let mut rng = Rng::new(seed ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    // Kept inside signed-64-bit range on purpose. Every competitor's integer
    // is signed: a full `u64` makes PostgreSQL's `COPY` reject the row, SQLite
    // silently store a *float* (losing precision), and MongoDB have no type
    // for it — so the systems would no longer hold the same value, which is
    // the one thing the shared dataset exists to guarantee.
    let score = rng.next_u64() >> 1;
    let kind = u32::try_from(rng.below(64)).unwrap_or(0);
    Thing {
        kind,
        score,
        name: format!("name-{n:010}"),
        tag: format!("tag-{:04}", n % 1024),
        body: body_text(&mut rng),
    }
}

/// The mutated form of record `n`, used by the update phase: same identity,
/// different bytes, same length class — so an update is a genuine whole-record
/// rewrite and not a resize in disguise.
#[must_use]
pub fn thing_v2(n: u64, seed: u64) -> Thing {
    thing_v2_at(n, seed, 0)
}

/// The `revision`-th update of row `n`.
///
/// The revision is not decoration. `thing_v2(n, seed)` is a pure function of
/// its arguments, so updating one row **twice** wrote byte-identical values —
/// and InnoDB turns an all-columns-unchanged `UPDATE` into a no-op. The update
/// phase draws uniformly with replacement, so at 50 000 updates over 100 000
/// rows roughly **21%** of MySQL's update phase was free work its peers
/// actually did. Salting by the draw ordinal makes every update a real write
/// on every system, which is the only way the column compares anything.
#[must_use]
pub fn thing_v2_at(n: u64, seed: u64, revision: u64) -> Thing {
    let mut t = thing(n, seed ^ 0xA5A5_A5A5_A5A5_A5A5 ^ revision);
    t.name = format!("name-{n:010}-{revision}");
    t.tag = format!("tag-{:04}", n % 1024);
    t
}

/// ~200 bytes of deterministic filler, in the shape of prose rather than
/// random noise — random bytes are the one input zstd (and every competitor's
/// compressor) cannot do anything with, which would flatter the uncompressed
/// systems in the footprint table.
fn body_text(rng: &mut Rng) -> String {
    const WORDS: [&str; 16] = [
        "record", "anchor", "segment", "chain", "instant", "tenant", "page",
        "journal", "commit", "barrier", "cache", "index", "pivot", "wire",
        "hash", "slot",
    ];
    let mut s = String::with_capacity(224);
    for _ in 0..32 {
        let w = WORDS[(rng.below(WORDS.len() as u64)) as usize];
        s.push_str(w);
        s.push(' ');
    }
    s
}

/// The summed wire size of the live dataset — the amplification denominator.
#[must_use]
pub fn logical_bytes(rows: u64, seed: u64) -> u64 {
    // Uniform by construction (fixed-width name/tag, 32 words of body), so a
    // sample is exact rather than an estimate; sample anyway in case the
    // generator ever grows a variable-length field.
    let sample = 256.min(rows);
    if sample == 0 {
        return 0;
    }
    let mut total = 0u64;
    for n in 0..sample {
        total += wavedb_core::to_wire(&thing(n, seed)).len() as u64;
    }
    total / sample * rows
}
