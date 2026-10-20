//! Output pinned to the published `seahash` 4.1.0.
//!
//! These vectors were recorded from the crate as published, before it was
//! vendored and rewritten without `unsafe` and as `const fn`. WaveDB's type
//! identities and shard placement are SeaHash outputs written to disk, so the
//! rewrite must reproduce the original bit for bit — every length class of
//! the tail handling (0–7, 8, 9–15, 16, 17–23, 24, 25–31) on both sides of
//! the 32-byte main loop, with the default and a chosen seed.

extern crate seahash;

use std::hash::Hasher;

/// The recorded input: `buf[n] = (n·131) ^ (n >> 3)`, truncated per case.
fn input() -> Vec<u8> {
    (0..300usize)
        .map(|n| (n.wrapping_mul(131) ^ (n >> 3)) as u8)
        .collect()
}

/// `(length, hash, hash_seeded(1, 2, 3, 4))`.
const GOLDEN: [(usize, u64, u64); 30] = [
    (0, 0xc920ca43256fdcb9, 0x32fc822c817a98b5),
    (1, 0x78d3db1f028ac5fd, 0xe1fea7ffd84020a8),
    (2, 0xba537befb06cf77e, 0x7c86caf2452bffa0),
    (3, 0xdc4cecc7c5514484, 0x39cdbb6fa9213336),
    (4, 0x3fab0e2bc1f74193, 0x2a6e51cb3086452f),
    (5, 0x6ef1c8274c17c8de, 0x29e07dfb404ca392),
    (6, 0x798d657722bda383, 0x428cda79b556b822),
    (7, 0xd93cf02de3fba3ee, 0x39b0f8cf3ed37935),
    (8, 0x84e203c653882579, 0xf6e59d766335eb43),
    (9, 0x9764ef16d81728ae, 0xae0274600c64742c),
    (15, 0x15adc2d7994bfc2d, 0x6c87f3f5baef85e9),
    (16, 0x7c066d2e1c9f211e, 0xf7101d7fe66a3f39),
    (17, 0x2b5a2a713eff8053, 0x85eeabbaf3ba6c96),
    (23, 0x3cdc73a6213c1358, 0xe92d30357295dc0c),
    (24, 0x94d353cb2f3aab35, 0x27e67f174671d3c7),
    (25, 0x98442c92b8a6fd0b, 0x28bca3b930f8fe07),
    (31, 0xc708ff549f8caed5, 0x8578a1a7c1d536f0),
    (32, 0xb757a0dd9a477736, 0x3d0f75066589c539),
    (33, 0x0d572075f487ef76, 0x654a9f4a9da5aa00),
    (40, 0xb2799951f752e17b, 0xb1e05274c14141fa),
    (63, 0xa8013181e9191f96, 0xcf9bae03efdab35b),
    (64, 0x89ac500c2f39f38e, 0x43f455584bef85d8),
    (65, 0x6b7bc6ffdd74842a, 0x847c49aa4f8ca6f1),
    (100, 0xbc6ea375cd7d0933, 0xfc56b5832b412ad6),
    (127, 0x41afa1e9c4894804, 0x1b851de648cdacc1),
    (128, 0x97574aa14cffcc9b, 0x84462f7108a0a7e7),
    (129, 0xc76520eb221dbe5c, 0x6f62ae0c73a0e096),
    (255, 0x5b6132bdfe1a39c2, 0xac1f3fd4bc531b7a),
    (256, 0x412b0856615038b1, 0x431648f9f1f60197),
    (299, 0x3167acc8452a4d82, 0xb5b26880e648fb59),
];

#[test]
fn hash_matches_the_published_crate() {
    let buf = input();
    for (len, plain, seeded) in GOLDEN {
        let b = &buf[..len];
        assert_eq!(seahash::hash(b), plain, "hash, len {len}");
        assert_eq!(
            seahash::hash_seeded(b, 1, 2, 3, 4),
            seeded,
            "seeded, len {len}"
        );
        assert_eq!(seahash::reference::hash(b), plain, "reference, len {len}");
    }
}

#[test]
fn the_streaming_hasher_matches_the_published_crate() {
    let buf = input();
    for (len, plain, _) in GOLDEN {
        // Fed in uneven pieces, so every tail path of `push_bytes` runs.
        let mut h = seahash::SeaHasher::new();
        for piece in buf[..len].chunks(7) {
            h.write(piece);
        }
        assert_eq!(h.finish(), plain, "stream, len {len}");
    }
}

/// `hash` is a `const fn`: a hash computed by the compiler equals the one
/// computed at run time.
#[test]
fn hash_runs_at_compile_time() {
    const AT_COMPILE_TIME: u64 = seahash::hash(b"to be or not to be");
    assert_eq!(AT_COMPILE_TIME, 1988685042348123509);
    assert_eq!(AT_COMPILE_TIME, seahash::hash(b"to be or not to be"));
}
