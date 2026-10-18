<div align="center"><img alt="Logo" width="200" src="./logo.png" /></div>
===================

SeaHash: A bizarrely fast hash function.

SeaHash is a hash function with performance better than (around 3-20% improvement) xxHash and
MetroHash. Furthermore, SeaHash has mathematically provable statistical guarantees.

In action:

[![The hash function in action.](http://ticki.github.io/img/seahash_construction_diagram.svg)](http://ticki.github.io/img/seahash_construction_diagram.svg)

## Thank you, Redox OS

This crate is SeaHash, by **ticki** and **Tom Almeida**, from the
[Redox OS](https://www.redox-os.org/) project
([gitlab.redox-os.org/redox-os/seahash](https://gitlab.redox-os.org/redox-os/seahash)).
WaveDB is built on it: every type identity (`STRUCT_HASH`) and every shard placement in WaveDB
is a SeaHash value written to disk, so this small, portable, well-reasoned function sits under
everything we store. A warm thank-you to ticki, to Tom Almeida, and to everyone at Redox OS who
designed it, proved its properties, and kept it maintained — and for releasing it under the MIT
license, which is what lets us carry it here.

## Why it lives in this repository

WaveDB vendors SeaHash 4.1.0 (the published crate, unchanged in output) for two reasons:

- **A current Rust.** The published crate is written for edition 2015 and reads its input
  through raw pointers. This copy builds as edition 2024 with the rest of the workspace, and
  every read is a plain, bounds-checked index — the crate is `#![forbid(unsafe_code)]`.
- **`const fn`.** `hash`, `hash_seeded` and the `State` operations are `const fn`, so a hash can
  be computed at compile time. WaveDB needs that to derive identities in `const` context from
  other crates' constants, where it previously had to use a different mixer.

Nothing else changed: the algorithm, the seeds and the output are the published 4.1.0, bit for
bit. `tests/golden.rs` pins that against vectors recorded from the published crate before it was
vendored, across every tail length class and on both sides of the 32-byte main loop; the
original tests (against the reference implementation, the streaming hasher, chunking, and the
quickcheck properties) and the criterion benches are kept as they were.

## License

MIT, as upstream — see [`LICENSE`](./LICENSE), copied verbatim from the upstream repository.
