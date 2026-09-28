# `bench-gen` — the fill/emit tool every seed is built with.
#
# ## Why the source is filtered, and why that needed a crate
#
# `bench-gen` is a build input of **every seed**, so whatever invalidates it
# invalidates the whole dataset tree. A derivation hashes every file in its
# source, compiled or not, so the only source that keeps a seed stable is one
# that holds *exactly* what the binary compiles from.
#
# That is why `bench-gen` lives in its own crate (`benches/gen`). Built from
# the whole checkout, an RFC edit or **recording a benchmark row** rebuilt all
# five seeds. Narrowed to the measuring crate's sources it was better and still
# wrong: `bench-gen` linked that crate's library, so editing a driver, the
# harness or a comment in it changed every seed's store path — measured, and at
# `large` that is a four-hour rebuild of data that did not change. Now the
# source is the gen crate, the engine crates it links, and the workspace root
# manifest they inherit from. Nothing in the measuring crate can reach it.
#
# The pairing with `params.nix`'s per-tier `rev` is deliberate: filtering
# stops unrelated edits from invalidating anything, and `rev` is how a human
# says "this dataset really did change" without waiting for a hash to notice.
{
  pkgs,
  rustPlatform,
  repoSrc,
}:
let
  inherit (pkgs) lib;
  inherit (lib.fileset) unions fileFilter toSource;

  rust = dir: fileFilter (f: f.hasExt "rs") dir;
  manifests = dir: fileFilter (f: f.name == "Cargo.toml") dir;
in
rustPlatform.buildRustPackage {
  pname = "bench-gen";
  version = "0.1.0";

  src = toSource {
    root = repoSrc;
    fileset = unions [
      # The gen crate: its sources, its manifest, and its own lock.
      (rust (repoSrc + "/benches/gen/src"))
      (repoSrc + "/benches/gen/Cargo.toml")
      (repoSrc + "/benches/gen/Cargo.lock")
      # The engine, by path dependency.
      (rust (repoSrc + "/crates"))
      (manifests (repoSrc + "/crates"))
      # Every crate manifest says `workspace = true` somewhere, so cargo
      # walks up to this one. Without it the path deps do not resolve.
      (repoSrc + "/Cargo.toml")
    ];
  };

  # The gen crate is its own workspace, so it carries its own lock — which is
  # what makes this build hermetic without reading the measuring crate's.
  cargoLock.lockFile = ../gen/Cargo.lock;
  # Both are needed: `cargoRoot` says where the lock to vendor from lives (the
  # root one is the workspace's, a different dependency set),
  # `buildAndTestSubdir` says what to build.
  cargoRoot = "benches/gen";
  buildAndTestSubdir = "benches/gen";
  doCheck = false;
}
